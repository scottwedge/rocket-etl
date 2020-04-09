import csv, json, requests, sys, traceback, re, time, math, operator, dataset
from datetime import datetime
from dateutil import parser
from pprint import pprint
from scourgify import normalize_address_record
import scourgify
from collections import OrderedDict, defaultdict, Counter
import phonenumbers
import pyproj

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


one_file = True # This controls whether the first schema and the job dicts
# are modified to allow all output to be dumped into one file.
one_file = False

unable_to_code = defaultdict(int)


features_by_address = defaultdict() # Geocoding cache
geo_db = dataset.connect('sqlite:///geocodes.db')
pelias_table = geo_db['pelias'] # An alternative to separate tables for separate geocoding sources would
# be one single table with the geocoder (and configuration) as one field and the results (in their
# myriad formats) as a nother.

# Network functions #
import socket

def get_an_ip_for_host(host):
    try:
        ips = socket.gethostbyname_ex(host)
    except socket.gaierror:
        return None
    return ips[0]

def site_is_up(site):
    """At present, geo.wprdc.org is only accessible from inside the Pitt network.
    Therefore, it's helpful to run this function to check whether this script
    should even try to do any geocoding."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2) # Add a two-second timeout to avoid interminable waiting.
    ip = get_an_ip_for_host(site)
    if ip is None:
        print("Unable to get an IP address for that host.")
        return False
    result = sock.connect_ex((ip,80))
    if result == 0:
        print('port OPEN')
        return True
    else:
        print('port CLOSED, connect_ex returned: '+str(result))
        return False

# End network functions #

def normalize(s):
    s = re.sub(',', '', s)
    s = s.strip()
    return re.sub("\s+", "_", s).lower()

def full_address(data):
    a = ''
    if 'street_address' in data and data['street_address'] is not None:
        a += f"{data['street_address'].strip()}, "
    if 'city' in data and data['city'] is not None:
        a += f"{data['city'].strip()}, "
    if 'state' in data and data['state'] is not None:
        a += f"{data['state'].strip()} "
    if 'zip_code' in data and data['zip_code'] is not None:
        a += f"{data['zip_code'].strip()}"
    return a

def synthesize_key(data, extra_fields=[]):
    # Note that phone number could still be added to all synthesized keys.
    assert 'name' in data
    best_address = full_address(data)
    parts = [data['name'], best_address]
    for f in extra_fields:
        parts.append(data[f])
    return normalize('::'.join(parts))

def distance(origin, destination):
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 6371 # km

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c

    return d

def geocode_strictly(full_address, verbose=True):
    """This function accesses a Pelias geocoder and parses the result. It only returns geocoordinates
    and other relevant result parameters if the geocoding is sufficiently precise (e.g., not a PO Box,
    not just a centroid)."""
    # In addition to determining geocoordinates, this could be used to possibly fill in or
    # standardize fields like ZIP code and city.
    global features_by_address
    if full_address in features_by_address: # Try the cache.
        features = features_by_address[full_address]
    else:
        rows = list(pelias_table.find(full_address=full_address))
        assert len(rows) < 2
        if len(rows) == 0: # If the local cache is empty, fetch results and store them.
            params = {'text': full_address}
            url = 'http://geo.wprdc.org/v1/search' # This URL currently seems to only work from within the Pitt network.
            r = requests.get(url, params=params)
            result = r.json()
            features = result['features']
            features_by_address[full_address] = features
            data = dict(full_address=full_address, features=json.dumps(features))
            pelias_table.upsert(data, ['full_address']) # The second parameter is the list of keys to use for the upsert.
        else: # Get the features from the local cache.
            features = json.loads(rows[0]['features']) # This is a brute force approach. Rather than paring the extremely long
            # results down to just what we need and what is useful, this stores everything.
            features_by_address[full_address] = features

    if len(features) == 1:
        feature = features[0]
    else:
        if verbose:
            print(f"{len(features)} found:")
        correct_state = [f for f in features if 'region_a' in f['properties'] and f['properties']['region_a'] == 'PA']
        if len(correct_state) == 0:
            if verbose:
                ic(full_address)
                try:
                    ic([f['properties']['label'] for f in features])
                except:
                    pprint([f['properties']['label'] for f in features])
            return None, None, None, None, None
        elif len(correct_state) == 1:
            feature = correct_state[0]
        else:
            correct_county = [f for f in correct_state if 'county' in f['properties'] and f['properties']['county'] == 'Allegheny County']
            if len(correct_county) == 0:
                if verbose:
                    ic(full_address)
                    try:
                        ic([f['properties']['label'] for f in correct_state])
                    except:
                        ic(correct_state)
                return None, None, None, None, None
            elif len(correct_county) == 1:
                feature = correct_county[0]
            else: # There are multiple possible locations in Allegheny County.
                if len(correct_county) == 2:
                    # How far apart are they?
                    coords = [f['geometry']['coordinates'] for f in correct_county]
                    d = distance(coords[0], coords[1])
                    if d > 0.5: # if distance is greater than 0.5 km.
                        if verbose:
                            ic(full_address)
                            print(f"The distance ({d} km) between these two possibilities ({[f['properties']['label'] for f in correct_county]}) is too large for confident geocoding.")
                        return None, None, None, None, None

                if verbose:
                    print(f"Picking the geocoding with the highest confidence out of the {len(correct_county)} options in the correct county ({[f['properties']['label'] for f in correct_county]}).")
                confidences = [f['properties']['confidence'] for f in correct_county]
                max_index, max_value = max(enumerate(confidences), key=operator.itemgetter(1))
                if Counter(confidences)[max_value] > 1:
                    if verbose:
                        print("There are multiple geocodings with the same confidence value, so just use the Pelias ordering (which should prioritize the best match type).")
                    feature = features[0]
                else:
                    feature = features[max_index]
                    if max_index != 0:
                        if verbose:
                            print(f"In this case, max_index is actually {max_index}.")

    properties = feature['properties']
    confidence = properties['confidence']
    if verbose:
        #ic(full_address)
        print(f"   Input:  {full_address}")
        print(f"   Output: {properties['label']}")
    match_type = properties['match_type']
    geometry = feature['geometry']
    if confidence <= 0.6:
        if verbose:
            print(f"Rejecting because confidence = {confidence}. Accuracy = {properties['accuracy']}. Layer = {properties['layer']}. Match type = {match_type}. Coordinates = {geometry['coordinates']}.")
        return None, None, None, None, None
    if confidence < 0.8:
        ic(result)
        raise ValueError(f"A confidence of {confidence} seems too low.")
    if 'county' in properties and properties['county'] != 'Allegheny County':
        if verbose:
            print(f"This location geocoded to {properties['county']}.")
        assert properties['county'] in ['Beaver County', 'Armstrong County', 'Butler County', 'Westmoreland County', 'Washington County']
        # For now, we'll allow these exceptions as the locations may be just across the border. Eventually all of these should be run down and checked.
    if verbose:
        print(f"   Confidence = {confidence}. Accuracy = {properties['accuracy']}. Layer = {properties['layer']}. Match type = {match_type}.")
    if properties['layer'] == 'postalcode':
        if verbose:
            print("Not returning the geocoordinates since this is a Post Office Box.")
        return None, None, None, None, None
    if properties['accuracy'] == 'centroid':
        if verbose:
            print("Rejecting because this is centroid accruacy.")
        return None, None, None, None, None
    if properties['accuracy'] != 'point':
        if verbose:
            ic(properties)

    if 'region_a' in properties:
        assert properties['region_a'] == 'PA'
    else:
        print(f"The lack of a 'region_a' value here strongly suggests that the result is bogus.")
        ic(properties)
        return None, None, None, None, None

    latitude = longitude = None
    if geometry['type'] == 'Point':
        longitude, latitude = geometry['coordinates']

    time.sleep(0.2)
    reduced_properties = {}
    fs = ['confidence', 'accuracy', 'match_type', 'layer', 'label']
    for f in fs:
        if f in properties:
            reduced_properties[f] = properties[f]

    return latitude, longitude, geometry, properties['county'] if 'county' in properties else None, reduced_properties

def centroid(vertexes):
    _x_list = [vertex [0] for vertex in vertexes]
    _y_list = [vertex [1] for vertex in vertexes]
    _len = len(vertexes)
    _x = sum(_x_list) / _len
    _y = sum(_y_list) / _len
    return(_x, _y)

def ntee_lookup(code):
    if code is None:
        return None
    filepath = ASSET_MAP_SOURCE_DIR + 'lookup.ntee2naics.csv'
    dr = csv.DictReader(open(filepath, 'r'))
    dicts = [row for row in dr]
    org_group_by_code = {r['NTEECC'] : r['NAME'] for r in dicts}
    if code == 'D30':
        ic(unable_to_code)
    if code in org_group_by_code:
        return org_group_by_code[code]
    if code[:-1] in org_group_by_code:
        return org_group_by_code[code[:-1]]
    unable_to_code[code] += 1
    return None

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

def standardize_phone_number(phone):
    if phone in [None, '', 'N/A', 'NONE', 'NA', 'NO PHONE']:
        return None
    maybe_phone = "+1" + re.sub(r'\D', '', phone)
    return str(phonenumbers.parse(maybe_phone).national_number)

class AssetSchema(pl.BaseSchema):
    synthesized_key = fields.String(default = '')

    class Meta:
        ordered = True

    @post_load
    def fix_synthesized_key(self, data):
        data['synthesized_key'] = synthesize_key(data)

class FarmersMarketsSchema(AssetSchema):
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

    data_source_name = fields.String(default="WPRDC Dataset: 2019 Farmer's Markets")
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/allegheny-county-farmers-markets-locations')
    # Add on fields just to this schema to cover every field used
    # in every other schema to ensure that they can all fit into
    # all-assets.csv when one_file == True.
    if one_file:
        accessibility = fields.String(default='', allow_none=True)
        additional_directions = fields.String(default='', allow_none=True)
        capacity = fields.Integer(load_from='seat_count', allow_none=True)
        county = fields.String(default='', allow_none=True)
        data_source_name = fields.String(default='', allow_none=True)
        data_source_url = fields.String(default='', allow_none=True)
        email = fields.String(default='', allow_none=True)
        full_address = fields.String(default='', allow_none=True)
        geometry = fields.String(default='', allow_none=True)
        geoproperties = fields.String(default='', allow_none=True)
        last_updated = fields.String(default='', allow_none=True)
        notes = fields.String(default='', allow_none=True)
        organization_name = fields.String(default='', allow_none=True)
        organization_phone = fields.String(default='', allow_none=True)
        parcel_id = fields.String(default='', allow_none=True)
        parent_location = fields.String(default='', allow_none=True)
        periodicity = fields.String(default='', allow_none=True)
        phone = fields.String(default='', allow_none=True)
        primary_key_from_rocket = fields.String(default='', allow_none=True)
        residence = fields.String(default='', allow_none=True)
        synthesized_key = fields.String(default='', allow_none=True)
        tags = fields.String(default='', allow_none=True)
        url = fields.String(default='', allow_none=True)

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

class FishFriesSchema(AssetSchema):
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
    county = fields.String(allow_none=True)
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
    data_source_name = fields.String(default="WPRDC Dataset: Pittsburgh Fish Fries (2020)")
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/pittsburgh-fish-fry-map')
    tags = fields.String(dump_only=True, default='fish fry')
    notes = fields.String(load_from='events', allow_none=True)

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
    def validate_coordinates(self, data):
        if float(data['latitude']) < 0:
            raise ValueError(f"{data['latitude']} is not a correct latitude value.")

    @pre_load
    def fix_booleans(self, data):
        booleans = ['handicap']
        for boolean in booleans:
            if data[boolean] == 'True':
                data[boolean] = True
            elif data[boolean] == 'False':
                data[boolean] = False

    @pre_load
    def fix_bogus_phone_numbers(self, data):
        # Since the fish fry events are being squeezed into the "community/non-profit organizations"
        # category, the hours of operation will be put into the notes field like so:
        if 'events' in data and data['events'] not in [None, '']:
            data['notes'] = f"Fish Fry events: {data['events']}"

        f = 'phone'
        if f in data:
            if data[f] in ['None', 'xxx-xxx-xxxx']:
                data[f] = None
            else:
                try:
                    standardized = standardize_phone_number(data[f])
                    data[f] = standardized
                except phonenumbers.phonenumberutil.NumberParseException:
                    print(f"phonenumbers through phonenumbers.phonenumberutil.NumberParseException on {data[f]}.")
                    standardized = None

                if standardized is None:
                    print(f'{data[f]} is not a valid phone number.')
                    if 'notes' not in data:
                        data['notes'] = ''
                    data['notes'] = 'THE PHONE NUMBER FIELD SHOULD HAVE THIS VALUE: {data[f]}. ' + data['notes']
                    data[f] = None

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

class LibrariesSchema(AssetSchema):
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
    data_source_name = fields.String(default="WPRDC Dataset: Library Locations (Carnegie Library of Pittsburgh)")
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/libraries')
    primary_key_from_rocket = fields.String(load_from='clpid', allow_none=False)

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

class FaithBasedFacilitiesSchema(AssetSchema):
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
    county = fields.String(default='', allow_none=True) # From geocoder.
    latitude = fields.Float(allow_none=True) # From geocoder.
    longitude = fields.Float(allow_none=True) # From geocoder.
    geometry = fields.String(default='', allow_none=True) # From geocoder.
    geoproperties = fields.String(default='', allow_none=True) # From geocoder.
    url = fields.String(load_from='website', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = fields.String(default="WPRDC Dataset: Library Locations (Carnegie Library of Pittsburgh)")
    #data_source_url = fields.String(default='https://data.wprdc.org/dataset/libraries')

    @pre_load
    def join_address(self, data):
        if 'address2' in data and data['address2'] not in [None, '']:
            data['street_address'] += ', ' + data['address2']

    @post_load
    def just_geocode_it(self, data):
        if to_geocode_or_not_to_geocode:
            data['latitude'], data['longitude'], data['geometry'], data['county'], data['geoproperties'] = geocode_strictly(full_address(data))
            #input("Press Enter to continue...")

class FamilySupportCentersSchema(AssetSchema):
    asset_type = fields.String(dump_only=True, default='family_support_centers')
    name = fields.String(load_from='center')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='facility_address_line', allow_none=True)
    city = fields.String(load_from='facility_city', allow_none=True)
    state = fields.String(load_from='facility_state', allow_none=True)
    zip_code = fields.String(load_from='facility_zip_code', allow_none=True)
    county = fields.String(default='', allow_none=True) # From geocoder.
    latitude = fields.Float(load_from='latitude', allow_none=True)
    longitude = fields.Float(load_from='longitude', allow_none=True)
    geometry = fields.String(default='', allow_none=True) # From geocoder.
    geoproperties = fields.String(default='', allow_none=True) # From geocoder.
    accuracy = fields.String(load_only=True, allow_none=True)
    organization_name = fields.String(load_from='lead_agency', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    #data_source_name = fields.String(default="WPRDC Dataset: Library Locations (Carnegie Library of Pittsburgh)")
    #data_source_url = fields.String(default='https://data.wprdc.org/dataset/libraries')
    #primary_key_from_rocket = fields.String(load_from='objectid', allow_none=True) # Possibly Unreliable

    @pre_load
    def filter_geocoding(self, data):
        if 'accuracy' not in data:
            data['latitude'], data['longitude'] = None, None
        elif data['accuracy'] not in ['EXACT_MATCH']:
            data['latitude'], data['longitude'] = None, None

    @post_load
    def maybe_geocode_it(self, data):
        if to_geocode_or_not_to_geocode:
            if data['latitude'] in [None, '']:
                data['latitude'], data['longitude'], data['geometry'], data['county'], data['geoproperties'] = geocode_strictly(full_address(data))

class SeniorCentersSchema(AssetSchema):
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
    geometry = fields.String()
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
    #data_source_name = fields.String(default="")
    #data_source_url = fields.String(default='')
    #primary_key_from_rocket = fields.String(load_from='id', allow_none=True) # Possibly Unreliable

    @pre_load
    def extract_coordinates(self, data):
        if data['geometry'] is not None:
            geometry = json.loads(data['geometry'])
            coordinates = geometry['coordinates']
            data['latitude'] = coordinates[1]
            data['longitude'] = coordinates[0]

class PollingPlacesSchema(AssetSchema):
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
    data_source_name = fields.String(default="WPRDC Dataset: Allegheny County Polling Place Locations (May 2018)")
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/allegheny-county-polling-place-locations-may-2018')
    #primary_key_from_rocket = fields.String(load_from='objectid_1', allow_none=True) # Possibly Unreliable
    mwd = fields.String(load_only=True)

    @post_load
    def fix_synthesized_key(self, data):
        data['synthesized_key'] = synthesize_key(data, ['mwd'])

class ACHACommunitySitesSchema(AssetSchema):
    asset_type = fields.String(dump_only=True, default='acha_community_sites')
    name = fields.String(load_from='name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(allow_none=True)
    #city = fields.String(allow_none=True)
    #state = fields.String(allow_none=True)
    zip_code = fields.String(allow_none=True)
    geometry = fields.String()
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
    data_source_name = fields.String(default='Allegheny County GIS: ACHA Sites')
    data_source_url = fields.String(default='http://services1.arcgis.com/vdNDkVykv9vEWFX4/arcgis/rest/services/ACHA_Sites/FeatureServer')
    #primary_key_from_rocket = fields.String(load_from='objectid', allow_none=True) # Possibly Unreliable

    @pre_load
    def extract_coordinates(self, data):
        if data['geometry'] is not None:
            geometry = json.loads(data['geometry'])
            coordinates = geometry['coordinates']
            data['latitude'] = coordinates[1]
            data['longitude'] = coordinates[0]

class ClinicsSchema(AssetSchema):
    asset_type = fields.String(dump_only=True, default='achd_clinics')
    name = fields.String(load_from='type_1')
    localizability = fields.String(dump_only=True, default='fixed')
    full_address = fields.String(load_from='match_addr', allow_none=True)
    street_address = fields.String(load_from='staddr', allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip_code = fields.String(load_from='zip', allow_none=True)
    latitude = fields.Float(load_from='y', allow_none=True)
    longitude = fields.Float(load_from='x', allow_none=True)
    #organization_name = fields.String(load_from='lead_agency', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    data_source_name = fields.String(default='Allegheny County GIS: ACHD Clinics')
    data_source_url = fields.String(default='https://services1.arcgis.com/vdNDkVykv9vEWFX4/arcgis/rest/services/ACHD_Clinics/FeatureServer')
    #primary_key_from_rocket = fields.String(load_from='objectid_1', allow_none=True) # Possibly Unreliable

    @pre_load
    def convert_from_state_plain(self, data):
        """Basically, we have to assume that we are in the Pennsylvania south plane
        or else not do the conversion."""
        if 'state' in data and data['state'] != 'PA':
            print("Unable to do this conversion.")
            data['x'] = None
            data['y'] = None
        elif -0.0001 < float(data['x']) < 0.0001 and -0.0001 < float(data['y']) < 0.0001:
            data['x'], data['y'] = None, None
        else:
            pa_south = pyproj.Proj("+init=EPSG:3365", preserve_units=True)
            wgs84 = pyproj.Proj("+init=EPSG:4326")
            try:
                data['x'], data['y'] = pyproj.transform(pa_south, wgs84, data['x'], data['y'])
            except TypeError:
                print(f"Unable to transform the coordinates {(data['x'], data['y'])}.")
                data['x'], data['y'] = None, None

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

class AffordableHousingSchema(AssetSchema):
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
    data_source_name = fields.String(default='Pennsylvania Housing Finance Agency: Allegheny County Inventory of Affordable Housing')
    data_source_url = fields.String(default='https://www.phfa.org/forms/multifamily_inventory/dv_allegheny.pdf')

    @pre_load
    def fix_phone(self, data):
        f = 'telephone'
        if data[f] not in [None, '']:
            data['phone'] = data[f]

class WICVendorsSchema(AssetSchema):
    asset_type = fields.String(dump_only=True, default='wic_vendors')
    name = fields.String(load_from='name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='arc_street', allow_none=True)
    city = fields.String(load_from='arc_city', allow_none=True)
    #state = fields.String(allow_none=True) # The 'ARC_State' field in the WIC data is empty.
    zip_code = fields.String(load_from='arc_zip', allow_none=True)
    latitude = fields.Float(load_from='latitude', allow_none=True)
    longitude = fields.Float(load_from='longitude', allow_none=True)
    #phone = fields.String(load_from='telephone', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    last_updated = fields.DateTime(default='2015-01-01') # It is apparently 2015 data.
    data_source_name = fields.String(default='Allegheny County WIC Vendor Locations')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/allegheny-county-wic-vendor-locations')
    #primary_key_from_rocket = fields.String(load_from='objectid', allow_none=True) # Possibly Unreliable

#    @pre_load
#    def convert_from_state_plain(self, data):
#        """Basically, we have to assume that we are in the Pennsylvania south plane
#        or else not do the conversion."""
#        if 'state' in data and data['state'] != 'PA':
#            print("Unable to do this conversion.")
#            data['x'] = None
#            data['y'] = None
#        elif -0.0001 < float(data['x']) < 0.0001 and -0.0001 < float(data['y']) < 0.0001:
#                data['x'], data['y'] = None, None
#            pa_south = pyproj.Proj("+init=EPSG:3365", preserve_units=True)
#            wgs84 = pyproj.Proj("+init=EPSG:4326")
#            try:
#                data['x'], data['y'] = pyproj.transform(pa_south, wgs84, data['x'], data['y'])
#            except TypeError:
#                print(f"Unable to transform the coordinates {(data['x'], data['y'])}.")
#                data['x'], data['y'] = None, None

class BigBurghServicesSchema(AssetSchema):
    name = fields.String(load_from='service_name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    #city = fields.String(load_from='address', allow_none=True)
    state = fields.String(load_from='address', allow_none=True) # This address is not a full address
    # as the city is omitted.
    zip_code = fields.String(load_from='address', allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    phone = fields.String(allow_none=True)
    organization_name = fields.String(load_from='organization', allow_none=True)
    additional_directions = fields.String(allow_none=True)
    hours_of_operation = fields.String(load_from='schedule', allow_none=True)
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    sensitive = fields.Boolean(allow_none=True)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    data_source_name = fields.String(default='WPRDC Datset: BigBurgh Social Service Listings: Services resource')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/bigburgh-social-service-listings')

    @pre_load
    def fix_address_fields(self, data):
        f = 'address'
        if data[f] in [None, '']:
            data['street_address'] = None
            data['state'] = None
            data['zip_code'] = None
            data['sensitive'] = True
        elif data[f] == "325 N Highland Avenue (across from Home Depot and Vento's Pizza), PA, 15206":
            data['street_address'] = '325 N Highland Avenue'
            data['city'] = 'Pittsburgh'
            data['state'] = 'PA'
            data['zip_code'] = '15206'
            data['sensitive'] = False
            data['additional_directions'] = "Across from Home Depot and Vento's Pizza"
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

class BigBurghPantriesSchema(BigBurghServicesSchema):
    job_code = 'bigburgh_pantries'
    asset_type = fields.String(dump_only=True, default='food_banks')
    notes = fields.String(allow_none=True)

    @pre_load
    def fix_notes(self, data):
        parts = []
        f = 'notes'
        g = 'narrative'
        if g in data and data[g] not in [None, 'NA', '']:
            parts.append(f"Narrative: {data[g]}{'.' if data[g].strip()[-1] != '.' else ''}")
        g = 'recommended_for'
        if g in data and data[g] not in [None, 'NA', '']:
            parts.append(f"Recommended for: {data[g]}.")
        g = 'requirements'
        if g in data and data[g] not in [None, 'NA', '']:
            parts.append(f"Requirements: {data[g]}.")
        data[f] = ' '.join(parts)

class BusStopsSchema(AssetSchema):
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

    data_source_name = fields.String(default='WPRDC Dataset: Port Authority Bus Stop Usage')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/port-authority-transit-stop-usage')
    primary_key_from_rocket = fields.String(load_from='stop_id', allow_none=True)
    stop_id = fields.String(load_only=True)

    @pre_load
    def fill_out_notes(self, data):
        # fix NAs
        fs = ['concat_latitude', 'concat_longitude', 'concat_stop_type', 'concat_shelter']

        for f in fs:
            if f in data and data[f] == "#N/A":
                data[f] = None

        data['notes'] = f"Routes: {data['concat_route']}, stop type: {data['concat_stop_type']}, shelter: {data['concat_shelter']}, 2019 average passengers off/day: {data['sum_fy19_avg_off']}, 2019 average passengers on/day: {data['sum_fy19_avg_on']}, 2019 total average passengers/day: {data['sum_fy19_avg_total']}"

    @post_load
    def fix_synthesized_key(self, data):
        data['synthesized_key'] = synthesize_key(data, ['stop_id'])

class CatholicSchema(AssetSchema):
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

    @pre_load
    def fix_datetime(self, data):
        # Since the fish fry events are being squeezed into the "community/non-profit organizations"
        # category, the hours of operation will be put into the notes field like so:
        if 'last_update' in data and data['last_update'] not in [None, '']:
            data['last_updated'] = parser.parse(data['last_update']).isoformat()

    @pre_load
    def fix_bogus_phone_numbers(self, data):
        f = 'phone_number'
        if f in data:
            if data[f] == 'N/A':
                data[f] = None

class MoreLibrariesSchema(AssetSchema):
    asset_type = fields.String(dump_only=True, default='libraries')
    name = fields.String(load_from='library')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(load_from='city_1', allow_none=True)
    state = fields.String(load_from='state_1', allow_none=True)
    county = fields.String(default='', allow_none=True) # From geocoder.
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    geometry = fields.Float(default='', allow_none=True) # From geocoder.
    child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource

    data_source_name = fields.String(default='Allegheny County Library Association: Library Finder')
    data_source_url = fields.String(default='https://aclalibraries.org/library-finder/')
    #primary_key_from_rocket = fields.String(load_from='objectid_12_13') # Possibly Unreliable

    @post_load
    def just_geocode_it(self, data):
        if to_geocode_or_not_to_geocode:
            data['latitude'], data['longitude'], data['geometry'], data['county'], _ = geocode_strictly(full_address(data))

class MuseumsSchema(AssetSchema):
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
    data_source_name = fields.String(default='Southwest Pennsylvania Commission GIS: Museums')
    data_source_url = fields.String(default='https://spcgis-spc.hub.arcgis.com/datasets/museums')
    #primary_key_from_rocket = fields.String(load_from='fid') # This is just the row number
    # so is considered unreliable and fragile.

class WICOfficesSchema(AssetSchema):
    # There are X and Y values in the source file, but they
    # are not longitude and latitude values.
    job_code = 'wic_offices'
    asset_type = fields.String(dump_only=True, default='wic_offices')
    name = fields.String(load_from='name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(load_from='arc_city', allow_none=True)
    state = fields.String(load_from='state_1', allow_none=True)
    zip_code = fields.String(load_from='zipcode', allow_none=True)
    latitude = fields.Float(load_from='y', allow_none=True)
    longitude = fields.Float(load_from='x', allow_none=True)
    phone = fields.String(load_from='phone_1', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    hours_of_operation = fields.String(load_from='officehours')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    data_source_name = fields.String(default='Allegheny County GIS')
    #data_source_url = fields.String(default='')
    primary_key_from_rocket = fields.String(load_from='objectid')

    @pre_load
    def convert_from_state_plain(self, data):
        """Basically, we have to assume that we are in the Pennsylvania south plane
        or else not do the conversion."""
        if 'state' in data and data['state'] != 'PA':
            print("Unable to do this conversion.")
            data['x'] = None
            data['y'] = None
        elif -0.0001 < float(data['x']) < 0.0001 and -0.0001 < float(data['y']) < 0.0001:
            data['x'], data['y'] = None, None
        else:
            pa_south = pyproj.Proj("+init=EPSG:3365", preserve_units=True)
            wgs84 = pyproj.Proj("+init=EPSG:4326")
            try:
                data['x'], data['y'] = pyproj.transform(pa_south, wgs84, data['x'], data['y'])
            except TypeError:
                print(f"Unable to transform the coordinates {(data['x'], data['y'])}.")
                data['x'], data['y'] = None, None

    @post_load
    def fix_key(self, data):
        assert hasattr(self, 'job_code')
        data['primary_key_from_rocket'] = form_key(self.job_code, data['primary_key_from_rocket'])

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

class RecCentersSchema(AssetSchema):
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
    data_source_name = fields.String(default='WPRDC Dataset: City of Pittsburgh Facilities')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/city-of-pittsburgh-facilities')
    primary_key_from_rocket = fields.String(load_from='id')

    @pre_load
    def join_address(self, data):
        if 'street' in data and data['street'] not in [None, '']:
            data['address_number'] += ' ' + data['street']

class FedQualHealthCentersSchema(AssetSchema):
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
    #data_source_name = fields.String(default='WPRDC Dataset: City of Pittsburgh Facilities')
    #data_source_url = fields.String(default='https://data.wprdc.org/dataset/city-of-pittsburgh-facilities')
    primary_key_from_rocket = fields.String(load_from='objectid')

    @pre_load
    def fix_coordinates(self, data):
        if 'geometry' in data and data['geometry'] not in [None, '']:
            geometry = json.loads(data['geometry'])
            coordinates = geometry['coordinates']
            data['latitude'] = coordinates[1]
            data['longitude'] = coordinates[0]

    def fix_bogus_phone_number(self, data):
        f = 'phone'
        if f in data and data[f] not in ['', None]:
            if len(data[f]) == 12 and data[f][:3] == '421':
                data[f] = re.sub('^421', '412', data[f])
                print("Changing 421 area code to 412.")

class PlacesOfWorshipSchema(AssetSchema):
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
    data_source_name = fields.String(default='Allegheny County GIS: Places of Worship')
    data_source_url = fields.String(default='http://alcogis.maps.arcgis.com/home/item.html?id=51cd2ffaea2e4579aace5a1d4f0de71f')

    @pre_load
    def join_address(self, data):
        f2 = 'address2'
        if f2 in data and data[f2] not in [None, '', 'NOT AVAILABLE']:
            data['address'] += ' ' + data[f2]

    @pre_load
    def fix_bogus_phone_numbers(self, data):
        f = 'phone_number'
        if f in data:
            if data[f] == 'N/A':
                data[f] = None

class ParkAndRidesSchema(AssetSchema):
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
    data_source_name = fields.String(default='WPRDC Dataset: Port Authority of Allegheny County Park and Rides')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/port-authority-of-allegheny-county-park-and-rides')

    @pre_load
    def fix_name(self, data):
        f = 'name'
        if f in data and data[f] not in [None, '']:
            data[f] += ' PARK & RIDE'

class PreschoolSchema(AssetSchema):
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
    #data_source_name = fields.String(default='WPRDC Dataset: Port Authority of Allegheny County Park and Rides')
    #data_source_url = fields.String(default='https://data.wprdc.org/dataset/port-authority-of-allegheny-county-park-and-rides')

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
            elif -0.0001 < float(data['x']) < 0.0001 and -0.0001 < float(data['y']) < 0.0001:
                data['x'], data['y'] = None, None
            else:
                pa_south = pyproj.Proj("+init=EPSG:3365", preserve_units=True)
                wgs84 = pyproj.Proj("+init=EPSG:4326")
                try:
                    data['x'], data['y'] = pyproj.transform(pa_south, wgs84, data['x'], data['y'])
                    data['latitude'] = float(data['y'])
                    data['longitude'] = float(data[f])
                except TypeError:
                    print(f"Unable to transform the coordinates {(data['x'], data['y'])}.")
                    data['x'], data['y'] = None, None

class SeniorCommunityCentersSchema(AssetSchema):
    asset_type = fields.String(dump_only=True, default='senior_centers')
    name = fields.String(load_from='name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip_code = fields.String(allow_none=True)
    county = fields.String(default='', allow_none=True) # From geocoder.
    latitude = fields.Float(allow_none=True) # From geocoder.
    longitude = fields.Float(allow_none=True) # From geocoder.
    geometry = fields.String(default='', allow_none=True) # From geocoder.
    phone = fields.String(load_from='phone_1', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    data_source_name = fields.String(default="Allegheny County web site: Senior Centers in Allegheny County")
    data_source_url = fields.String(default='https://www.alleghenycounty.us/Human-Services/Programs-Services/Older-Adults/Senior-Centers.aspx')

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

    @post_load
    def just_geocode_it(self, data):
        if to_geocode_or_not_to_geocode:
            data['latitude'], data['longitude'], data['geometry'], data['county'], _ = geocode_strictly(full_address(data))

class PublicBuildingsSchema(AssetSchema):
    asset_type = fields.String(dump_only=True, default='public_buildings')
    name = fields.String(load_from='facility')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(allow_none=True)
    #state = fields.String(allow_none=True)
    zip_code = fields.String(load_from='zipcode', allow_none=True)

    county = fields.String(default='', allow_none=True) # From geocoder.
    latitude = fields.Float(allow_none=True) # From geocoder.
    longitude = fields.Float(allow_none=True) # From geocoder.
    geometry = fields.String(default='', allow_none=True) # From geocoder.
    #phone = fields.String(load_from='phone_1', allow_none=True)
    #additional_directions = fields.String(allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    data_source_name = fields.String(default="WPRDC Dataset: Allegheny County Public Building Locations")
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/allegheny-county-public-building-locations')

    @pre_load
    def join_name(self, data):
        f = 'facility_c'
        if f in data and data[f] not in [None, '']:
            data['facility'] += f' ({data[f]})'

    @post_load
    def just_geocode_it(self, data):
        if to_geocode_or_not_to_geocode:
            data['latitude'], data['longitude'], data['geometry'], data['county'], _ = geocode_strictly(full_address(data))

class VAFacilitiesSchema(AssetSchema):
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
    data_source_name = fields.String(default='GIS: Veterans Health Administration Medical Facilities')
    data_source_url = fields.String(default='https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Veterans_Health_Administration_Medical_Facilities/FeatureServer')

    @pre_load
    def join_address(self, data):
        if 'address2' in data and data['address2'] not in [None, '', ' ', 'NOT AVAILABLE']:
            data['address'] += ', ' + data['address2']

    @pre_load
    def join_name(self, data):
        f = 'facility_c'
        if f in data and data[f] not in [None, '']:
            data['facility'] += f' ({data[f]})'

class VetSocialOrgsSchema(AssetSchema):
    asset_type = fields.String(dump_only=True, default='veterans_social_orgs')
    name = fields.String(load_from='title')
    localizability = fields.String(dump_only=True, default='fixed')
    full_address = fields.String(load_from='fixed_address', allow_none=True)
    street_address = fields.String(load_from='address', allow_none=True)
    city = fields.String(load_from='address', allow_none=True)
    state = fields.String(load_from='address', allow_none=True)
    zip_code = fields.String(load_from='address', allow_none=True)

    county = fields.String(default='', allow_none=True) # From geocoder.
    latitude = fields.Float(allow_none=True) # From geocoder.
    longitude = fields.Float(allow_none=True) # From geocoder.
    geometry = fields.String(default='', allow_none=True) # From geocoder.
    geoproperties = fields.String(default='', allow_none=True) # From geocoder.
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource
    data_source_name = fields.String(default="Allegheny County GIS: Veteran's Social Organizations")
    data_source_url = fields.String(default='https://services1.arcgis.com/vdNDkVykv9vEWFX4/arcgis/rest/services/VeteransSocialOrganizations/FeatureServer')

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

    @post_load
    def just_geocode_it(self, data):
        if to_geocode_or_not_to_geocode:
            data['latitude'], data['longitude'], data['geometry'], data['county'], data['geoproperties'] = geocode_strictly(full_address(data))

class NursingHomesSchema(AssetSchema):
    # In addition to contact phone number, the source file lists
    # the name of a contact person.
    job_code = 'nursing_homes'
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
    last_updated = fields.DateTime(default='2018-01-01') # Since it is 2018 data.
    data_source_name = fields.String(default="PASDA: Nursing Homes")
    data_source_url = fields.String(default='http://www.pasda.psu.edu/uci/DataSummary.aspx?dataset=3074')
    primary_key_from_rocket = fields.String(load_from='facility_i', allow_none=False)

    @post_load
    def fix_key(self, data):
        assert hasattr(self, 'job_code')
        data['primary_key_from_rocket'] = form_key(self.job_code, data['primary_key_from_rocket'])

class LicenseSchema(AssetSchema):
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
    data_source_name = fields.String(default="Pennsylvania Licensing System")
    data_source_url = fields.String(default='https://www.pals.pa.gov/#/page/search')
    primary_key_from_rocket = fields.String(load_from='licensenumber', allow_none=False)

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

    @post_load
    def fix_key(self, data):
        assert hasattr(self, 'job_code')
        data['primary_key_from_rocket'] = form_key(self.job_code, data['primary_key_from_rocket'])

class DentistsSchema(LicenseSchema):
    job_code = 'dentists'
    asset_type = fields.String(dump_only=True, default='dentists')

class BarbersSchema(LicenseSchema):
    job_code = 'barbers'
    asset_type = fields.String(dump_only=True, default='barbers')

class NailSalonsSchema(LicenseSchema):
    job_code = 'nail_salons'
    asset_type = fields.String(dump_only=True, default='nail_salons')

class HairSalonsSchema(LicenseSchema):
    job_code = 'hair_salons'
    asset_type = fields.String(dump_only=True, default='hair_salons')

class PharmaciesSchema(LicenseSchema):
    job_code = 'pharmacies'
    asset_type = fields.String(dump_only=True, default='pharmacies')

class WMDSchema(AssetSchema):
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
    data_source_name = fields.String(default="WPRDC Dataset: Allegheny County Weights and Measures Inspections")
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/allegheny-county-weights-and-measures-inspections')
    primary_key_from_rocket = fields.String(load_from='store_id', allow_none=False)

    @pre_load
    def fix_phone(self, data):
        f = 'business_phone'
        if f in data and data[f] in ['', ' ', 'NOT AVAILABLE', 'NULL', 'NO PHONE #', 'NA', 'N/A', 'NO PHONE YET', 'NONE']:
            data[f] = None

    @post_load
    def fix_key(self, data):
        assert hasattr(self, 'job_code')
        data['primary_key_from_rocket'] = form_key(self.job_code, data['primary_key_from_rocket'])

class LaundromatsSchema(WMDSchema):
    job_code = 'laundromats'
    asset_type = fields.String(dump_only=True, default='laundromats')

class GasVendorsSchema(WMDSchema):
    job_code = 'gas_vendors'
    asset_type = fields.String(dump_only=True, default='gas_stations')

class WMDCoffeeShopsSchema(WMDSchema):
    job_code = 'wmd_coffee_shops'
    asset_type = fields.String(dump_only=True, default='coffee_shops')

class ChildCareCentersSchema(AssetSchema):
    job_code = 'child_care_providers'
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
    data_source_name = fields.String(default="opendata PA Dataset: Child Care Providers Listing Current Monthly Facility County Human Services")
    data_source_url = fields.String(default='https://data.pa.gov/Early-Education/Child-Care-Providers-Listing-Current-Human-Service/ajn5-kaxt')
    primary_key_from_rocket = fields.String(load_from='license_number')

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

    @post_load
    def fix_key(self, data):
        assert hasattr(self, 'job_code')
        data['primary_key_from_rocket'] = form_key(self.job_code, data['primary_key_from_rocket'])

class PropertyAssessmentsSchema(AssetSchema):
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
    data_source_name = fields.String(default='WPRDC Dataset: Allegheny County Property Assessments')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/property-assessments')
    primary_key_from_rocket = fields.String(load_from='parid')

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

    @post_load
    def fix_key(self, data):
        assert hasattr(self, 'job_code')
        data['primary_key_from_rocket'] = form_key(self.job_code, data['primary_key_from_rocket'])

class ApartmentsSchema(PropertyAssessmentsSchema):
    job_code = 'apartments'
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

class UniversitiesSchema(AssetSchema):
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
    data_source_name = fields.String(default='Southwestern Pennsylvania Commission: Universities')
    data_source_url = fields.String(default='https://spcgis-spc.hub.arcgis.com/datasets/universities?geometry=-80.756%2C40.423%2C-79.450%2C40.606')

class SchoolsSchema(AssetSchema):
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
    data_source_name = fields.String(default='Open Data PA Dataset: Public and Private Education Institutions 2017 Education')
    data_source_url = fields.String(default='https://data.pa.gov/Schools-that-Teach/Public-and-Private-Education-Institutions-2017-Edu/ccgi-a8qm')

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

class ParkFacilitiesSchema(AssetSchema):
    job_code = 'county_park_facilities'
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
    data_source_name = fields.String(default='WPRDC Dataset: Allegheny County Park Facilities')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/allegheny-county-park-facilities')
    primary_key_from_rocket = fields.String(load_from='globalid')
    globalid = fields.String(load_only=True)

    @pre_load
    def fix_nones(self, data):
        fs = ['name', 'park', 'type']
        for f in fs:
            if f in data and data[f] in [None, '', ' ']:
                if f == 'name':
                    data[f] = '(Unnamed location)'
                else: # park and type (which maps to parent_location and tags)
                    data[f] = None

    @post_load
    def fix_key(self, data):
        assert hasattr(self, 'job_code')
        data['primary_key_from_rocket'] = form_key(self.job_code, data['primary_key_from_rocket'])

    @post_load
    def fix_synthesized_key(self, data):
        data['synthesized_key'] = synthesize_key(data, ['globalid'])


class CityParksSchema(AssetSchema):
    job_code = 'city_parks'
    asset_type = fields.String(dump_only=True, default='parks_and_facilities')
    name = fields.String(load_from='updatepknm', allow_none=False)
    #parent_location = fields.String(load_from='park', allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    #organization_name = fields.String(default='Allegheny County Parks Department')
    notes = fields.String(load_from='maintenanc', allow_none=True)
    tags = fields.String(load_from='final_cat', allow_none=True)
    #additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, allow_none=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    geometry = fields.String(load_from='geometry')
    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    localizability = fields.String(dump_only=True, default='fixed')
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    last_updated = fields.DateTime(load_from='last_edi_1', allow_none=True)
    data_source_name = fields.String(default='WPRDC Dataset: Pittsburgh Parks')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/pittsburgh-parks')
    primary_key_from_rocket = fields.String(load_from='globalid_2')

    @pre_load
    def fix_notes(self, data):
        fs = ['maintenanc']
        for f in fs:
            if f in data and data[f] not in [None, '', ' ']:
                data[f] = 'Maintenance: ' + data[f]

    @pre_load
    def calculate_centroid(self, data):
        f = 'geometry'
        if f in data and data[f] not in ['{}', None, '']:
            geom = json.loads(data[f])
            if 'type' in geom:
                if geom['type'] == 'Polygon':
                    assert len(geom['coordinates']) == 1
                    data['longitude'], data['latitude'] = centroid(geom['coordinates'][0])
                else:
                    print(f" * Currently unprepared to calculate the centroid for a {geom['type']} geometry.")

    @post_load
    def fix_key(self, data):
        assert hasattr(self, 'job_code')
        data['primary_key_from_rocket'] = form_key(self.job_code, data['primary_key_from_rocket'])

class CityPlaygroundsSchema(AssetSchema):
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
    data_source_name = fields.String(default='WPRDC Dataset: Playground Equipment')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/playground-equipment')

    @pre_load
    def fix_address(self, data):
        f0 = 'median_street_number'
        f = 'median_street'
        if f0 not in data or data[f0] in [None, '', ' ']:
            data[f0] = data[f]
        elif f in data and data[f] not in [None, '', ' ']:
            data[f0] += ' ' + data[f]

class CityPlaygroundEquipmentSchema(AssetSchema):
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
    accessibility = fields.Boolean(load_from='ada_accessible', allow_none=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    notes = fields.String(dump_only=True, default='This is derived from an aggregated version of the WPRDC Playground Equipment dataset.')
    #geometry = fields.String()
    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    localizability = fields.String(dump_only=True, default='fixed')
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = fields.DateTime(load_from='last_edi_1', allow_none=True)
    data_source_name = fields.String(default='WPRDC Dataset: Playground Equipment')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/playground-equipment')
    primary_key_from_rocket = fields.String(load_from='id')
    id = fields.String(load_only=True)

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
    def fix_accessibility(self, data):
        f = 'accessibility'
        if f in data and data[f] not in [None, '', ' ']:
            if data[f] == 't':
                data[f] = True
            elif data[f] == 'f':
                data[f] = False
            else:
                data[f] = None

    @pre_load
    def fix_address(self, data):
        f0 = 'street_number'
        f = 'street'
        if f0 not in data or data[f0] in [None, '', ' ']:
            data[f0] = data[f]
        elif f in data and data[f] not in [None, '', ' ']:
            data[f0] += ' ' + data[f]

    @post_load
    def fix_synthesized_key(self, data):
        data['synthesized_key'] = synthesize_key(data, ['id'])

class GeocodedFoodFacilitiesSchema(AssetSchema):
    name = fields.String(load_from='facility_name', allow_none=False)
    #parent_location = fields.String(load_from='name', allow_none=True)
    street_address = fields.String(load_from='num', allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip_code = fields.String(load_from='zip', allow_none=True)
    latitude = fields.Float(load_from='y', allow_none=True)
    longitude = fields.Float(load_from='x', allow_none=True)
    #organization_name = fields.String(default='Allegheny County Parks Department')
    #tags = fields.String(load_from='final_cat', allow_none=True)
    #additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)
    capacity = fields.Integer(load_from='seat_count', allow_none=True)

    #notes = fields.String(dump_only=True, default='This is derived from an aggregated version of the WPRDC Playground Equipment dataset.')
    #geometry = fields.String()
    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    localizability = fields.String(dump_only=True, default='fixed')
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = fields.DateTime(load_from='last_edi_1', allow_none=True)
    data_source_name = fields.String(default='WPRDC Dataset: Allegheny County Restaurant/Food Facility Inspections and Locations: Geocoded Food Facilities')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/allegheny-county-restaurant-food-facility-inspection-violations/resource/112a3821-334d-4f3f-ab40-4de1220b1a0a')
    primary_key_from_rocket = fields.String(load_from='id')

    @pre_load
    def fix_address(self, data):
        f0 = 'num'
        f = 'street'
        if f0 not in data or data[f0] in [None, '', ' ', 'NA']:
            data[f0] = data[f]
        elif f in data and data[f] not in [None, '', ' ']:
            data[f0] += ' ' + data[f]

    @pre_load
    def fix_nas(self, data):
        fs = ['zip', 'city', 'state']

        for f in fs:
            if f in data and data[f] in ["NA", '']:
                data[f] = None

class GeocodedRestaurantsSchema(GeocodedFoodFacilitiesSchema):
    asset_type = fields.String(dump_only=True, default='restaurants')

class GeocodedSupermarketsSchema(GeocodedFoodFacilitiesSchema):
    asset_type = fields.String(dump_only=True, default='supermarkets')

class GeocodedFoodBanksSchema(GeocodedFoodFacilitiesSchema):
    asset_type = fields.String(dump_only=True, default='food_banks')

class GeocodedSocialClubsSchema(GeocodedFoodFacilitiesSchema):
    asset_type = fields.String(dump_only=True, default='bars')
    tags = fields.String(dump_only=True, default='social club')

class PrimaryCareSchema(AssetSchema):
    job_code = 'primary_care'
    asset_type = fields.String(dump_only=True, default='doctors_offices')
    name = fields.String(load_from='group_name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='practice_addr_1')
    city = fields.String(load_from='practice_city')
    state = fields.String(load_from='practice_state')
    zip_code = fields.String(load_from='practice_zip')
    latitude = fields.Float(load_from='latitude', allow_none=True)
    longitude = fields.Float(load_from='longitude', allow_none=True)
    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource # 2014
    data_source_name = fields.String(default='WPRDC Data: Primary Care Access 2014 Data')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/allegheny-county-primary-care-facilities/resource/a11c31cf-a116-4076-8475-c4f185358c2d')
    #primary_key_from_rocket = fields.String(load_from='clpid', allow_none=False)

    @pre_load
    def join_address(self, data):
        f2 = 'practice_addr_2'
        if f2 in data and data[f2] not in [None, '', 'NOT AVAILABLE']:
            data['practice_addr_1'] += ', ' + data[f2]

class IRSGeocodedSchema(AssetSchema):
    asset_type = fields.String(dump_only=True, default='community_nonprofit_orgs')
    name = fields.String(load_from='name', allow_none=False)
    #parent_location = fields.String(load_from='name', allow_none=True)
    street_address = fields.String(load_from='street', allow_none=True) # In the source file,
    # there are 41 rows where Bob has edited arc_street to make it more readily geocodable,
    # while 'street' contains the original address, with suite numbers of whatever.
    city = fields.String()
    state = fields.String()
    zip_code = fields.String(load_from='zip', allow_none=True)
    latitude = fields.Float(load_from='latitude', allow_none=True)
    longitude = fields.Float(load_from='longitude', allow_none=True)
    #organization_name = fields.String(default='Allegheny County Parks Department')
    #tags = fields.String(load_from='final_cat', allow_none=True)
    #additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    notes = fields.String(load_from='notes', allow_none=True)
    #geometry = fields.String()
    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    localizability = fields.String(dump_only=True, default='fixed')
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = fields.DateTime(load_from='last_edi_1', allow_none=True)
    data_source_name = fields.String(default='IRS: Exempt Organizations Business Master File Extract')
    data_source_url = fields.String(default='https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf')
    primary_key_from_rocket = fields.String(load_from='ein')
    ein = fields.String(load_only=True)

    @pre_load
    def fix_address(self, data):
        f0 = 'num'
        f = 'street'
        if f0 not in data or data[f0] in [None, '', ' ']:
            data[f0] = data[f]
        elif f in data and data[f] not in [None, '', ' ']:
            data[f0] += ' ' + data[f]

    @pre_load
    def fix_notes(self, data):
        f0 = 'ntee_cd'
        f = 'notes'
        if f0 in data or data[f0] not in [None, '', ' ']:
            data[f] = ntee_lookup(data[f0])

    #@pre_load
    #def convert_from_state_plain(self, data):
    #    """Basically, we have to assume that we are in the Pennsylvania south plane
    #    or else not do the conversion."""
    #    if 'state' in data and data['state'] != 'PA':
    #        print("Unable to do this conversion.")
    #        data['x'] = None
    #        data['y'] = None
    #    else:
    #       if -0.0001 < float(data['x']) < 0.0001 and -0.0001 < float(data['y']) < 0.0001:
    #            data['x'], data['y'] = None, None
    #        pa_south = pyproj.Proj("+init=EPSG:3365", preserve_units=True)
    #        wgs84 = pyproj.Proj("+init=EPSG:4326")
    #        try:
    #            data['x'], data['y'] = pyproj.transform(pa_south, wgs84, data['x'], data['y'])
    #        except TypeError:
    #            print(f"Unable to transform the coordinates {(data['x'], data['y'])}.")
    #            data['x'], data['y'] = None, None

    @post_load
    def fix_synthesized_key(self, data):
        data['synthesized_key'] = synthesize_key(data, ['ein'])

#def conditionally_get_city_files(job, **kwparameters):
#    if not kwparameters['use_local_files']:
#        fetch_city_file(job)

class LiquorLicensesSchema(AssetSchema):
    #asset_type = fields.String(dump_only=True, default='community_nonprofit_orgs')
    name = fields.String(load_from='premises', allow_none=False)
    #parent_location = fields.String(load_from='name', allow_none=True)
    premises_address = fields.String(load_only=True) # Can we split this into street_address, city, state, and zip_code? Try using usaddresses.
    street_address = fields.String(allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip_code = fields.String(allow_none=True)
    #latitude = fields.Float(load_from='latitude', allow_none=True)
    #longitude = fields.Float(load_from='longitude', allow_none=True)
    organization_name = fields.String(load_from='licensee', allow_none=True)
    #tags = fields.String(load_from='final_cat', allow_none=True)
    #additional_directions = fields.String(load_from='shopping_center', allow_none=True)
    #url = fields.String(load_from='facility_u', allow_none=True)
    #hours_of_operation = fields.String(load_from='day_time')
    #child_friendly = fields.String(dump_only=True, default=True)
    #computers_available = fields.String(dump_only=True, allow_none=True, default=False)

    notes = fields.String(allow_none=True)
    #geometry = fields.String()
    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    localizability = fields.String(dump_only=True, default='fixed')
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = fields.DateTime(load_from='last_edi_1', allow_none=True)
    data_source_name = fields.String(default='Pennsylvania Liquor Control Board: Licenses')
    data_source_url = fields.String(default='https://plcbplus.pa.gov/pub/Default.aspx?PossePresentation=LicenseSearch')
    primary_key_from_rocket = fields.String(load_from='lid')

    @pre_load
    def fix_notes(self, data):
        notes_list = []
        f_type = 'license_type'
        if f_type in data:
            notes_list.append(f"Liquor license type: {data[f_type]}")
        f_status = 'status'
        if f_status in data:
            notes_list.append(f"Liquor license status: {data[f_status]}")
        f_issued = 'last_issued_date'
        if f_issued in data and data[f_issued] not in ['', None, ' ', 'NA']:
            notes_list.append(f"Last issued: {data[f_issued]}")
        f_expiration = 'expiration_date'
        if f_expiration in data:
            notes_list.append(f"Expires: {data[f_expiration]}")

        f_address = 'premises_address'
        if f_address in data:
            notes_list.append(f"Full address: {data[f_address]}")

        if len(notes_list) > 0:
            data['notes'] = ', '.join(notes_list)


    @pre_load
    def parse_address(self, data):
        f = 'premises_address'
        # First deal with difficult-to-parse addresses
        if data[f] == 'PENNSYLVANIA AVE CAPITAL ST  WHITE OAK, MCKEESPORT PA 15131':
            data['street_address'] = 'PENNSYLVANIA AVE & CAPITAL ST'
            data['city'] = 'MCKEESPORT'
            data['state'] = 'PA'
            data['zip_code'] = '15131'
        elif data[f] == 'NOBLESTOWN RD P O BOX 280, STURGEON PA 15082-0280':
            data['street_address'] = 'NOBLESTOWN RD'
            data['city'] = 'STURGEON'
            data['state'] = 'PA'
            data['zip_code'] = '15082'
        elif data[f] == 'CAMPBELL ST PO BOX 454, CUDDY PA 15031-0454':
            data['street_address'] = 'CAMPBELL ST'
            data['city'] = 'CUDDY'
            data['state'] = 'PA'
            data['zip_code'] = '15031'
        elif data[f] == 'MCCLELLAND RD, INDIANOLA PA 15051-9731':
            data['street_address'] = 'MCCLELLAND RD'
            data['city'] = 'INDIANOLA'
            data['state'] = 'PA'
            data['zip_code'] = '15031'
        elif data[f] == 'WINDMERE RD BEN AVON HEIGHTS, PITTSBURGH PA 15202':
            data['street_address'] = 'WINDMERE RD'
            data['city'] = 'BEN AVON HEIGHTS'
            data['state'] = 'PA'
            data['zip_code'] = '15202'
        elif data[f] == 'LITTLE DEER CREEK VALLEY RD PO BOX 461, RUSSELLTON PA 15076-0461':
            data['street_address'] = 'LITTLE DEER CREEK VALLEY RD'
            data['city'] = 'RUSSELLTON'
            data['state'] = 'PA'
            data['zip_code'] = '15076'
        elif data[f] == '1910-12 SOUTH ST NORTH BRADDOCK, BRADDOCK PA 15104':
            data['street_address'] = '1910-12 SOUTH ST'
            data['city'] = 'BRADDOCK'
            data['state'] = 'PA'
            data['zip_code'] = '15104'
        else:
            try:
                parsed = normalize_address_record(data[f])
                #assert detected_type == 'Street Address'
                data['street_address'] = parsed['address_line_1']
                if 'address_line_2' in parsed and parsed['address_line_2'] not in [None, '']:
                    data['street_address'] += ', ' + parsed['address_line_2']
                data['city'] = parsed.get('city', None)
                if data['city'].count(', ') == 1:
                    data['city'] = re.sub(', PITTSBURGH', '', data['city'])
                data['state'] = parsed.get('state', None)
                data['zip_code'] = parsed.get('postal_code', None)
                failed = False
            except scourgify.exceptions.UnParseableAddressError:
                failed = True
            if 'street_address' in data and 'city' in data and data['street_address'] == data['city']:
                failed = True

            if failed:
                # Try to pull out the PO BOX and parse around it.
                problematic_address = re.sub(' P\s?O BOX \d+,', ',', data[f])
                if problematic_address == data[f]:
                    problematic_address = re.sub(' BOX \d+,', ',', data[f])
                if problematic_address != data[f]:
                    try:
                        parsed = normalize_address_record(problematic_address)
                        #assert detected_type == 'Street Address'
                        data['street_address'] = parsed['address_line_1']
                        if 'address_line_2' in parsed and parsed['address_line_2'] not in [None, '']:
                            data['street_address'] += ', ' + parsed['address_line_2']
                        data['city'] = parsed.get('city', None)
                        data['state'] = parsed.get('state', None)
                        data['zip_code'] = parsed.get('postal_code', None)
                        failed = False
                    except scourgify.exceptions.UnParseableAddressError:
                        failed = True
                if problematic_address == data[f] or failed:
                    if data[f].count(',') == 1:
                        data['street_address'], city_state_zip = data[f].split(', ')
                        data['city'], data['zip_code'] = city_state_zip.split(' PA ')
                        data['state'] = 'PA'
                        failed = False
                if failed:
                    ic(data[f])
                    raise ValueError(f"Unable to parse {data[f]}")

            if data['city'].count(', ') == 1:
                _, data['city'] = data['city'].split(', ')
                failed = False

class LiquorSocialClubsSchema(LiquorLicensesSchema):
    asset_type = fields.String(dump_only=True, default='bars')
    tags = fields.String(dump_only=True, default='social club')

class PostOfficesSchema(AssetSchema):
#    asset_category = fields.String(dump_only=True, default='Civic')
    asset_type = fields.String(dump_only=True, default='post_offices')
    name = fields.String(load_from='po_name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='property_address')
    city = fields.String()
    state = fields.String(load_from='st')
    zip_code = fields.String()
    #latitude = fields.Float(load_from='latitude', allow_none=True)
    #longitude = fields.Float(load_from='longitude', allow_none=True)
    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource # 2014
    data_source_name = fields.String(default='USPS Owned Facilities Reports')
    data_source_url = fields.String(default='https://about.usps.com/who/legal/foia/owned-facilities.htm')
    primary_key_from_rocket = fields.String(load_from='fdb_id_(all)', allow_none=False)

    @pre_load
    def fix_name(self, data):
        f2 = 'unit_name'
        f = 'po_name'
        if f2 in data and data[f2] not in [None, '', 'NOT AVAILABLE']:
            data[f] += ' POST OFFICE - ' + data[f2]

class FDICSchema(AssetSchema):
#    asset_category = fields.String(dump_only=True, default='Business')
    asset_type = fields.String(dump_only=True, default='banks')
    name = fields.String(load_from='name')
    localizability = fields.String(dump_only=True, default='fixed')
    street_address = fields.String(load_from='address')
    city = fields.String()
    state = fields.String(load_from='stalp')
    zip_code = fields.String(load_from='zip')
    #latitude = fields.Float(load_from='latitude', allow_none=True)
    #longitude = fields.Float(load_from='longitude', allow_none=True)
    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource # 2014
    data_source_name = fields.String(default='FDIC Bank Data API Developer Portal')
    data_source_url = fields.String(default='https://banks.data.fdic.gov/docs/')
    primary_key_from_rocket = fields.String(load_from='uninum', allow_none=False)
    notes = fields.String(allow_none=True)

    @pre_load
    def fix_name(self, data):
        f2 = 'offname'
        f = 'name'
        if f2 in data and data[f2] not in [None, '', 'NOT AVAILABLE']:
            data[f] += ' (' + data[f2] + ')'

    @pre_load
    def fix_notes(self, data):
        f = 'servtype'
        service_by_code = {'11': 'Full Service Brick and Mortar Office',
                '12': 'Full Service Retail Office',
                '13': 'Full Service Cyber Office',
                #'14': 'Full Service Mobile Office',
                #'15': 'Full Service Home/Phone Banking',
                '21': 'Limited Service Administrative Office',
                '22': 'Limited Service Military Facility',
                '23': 'Limited Service Facility Office',
                '24': 'Limited Service Loan Production Office',
                '25': 'Limited Service Consumer Credit Office',
                '26': 'Limited Service Contractual Office',
                '27': 'Limited Service Messenger Office',
                '28': 'Limited Service Retail Office',
                '29': 'Limited Service Mobile Office',
                '30': 'Limited Service Trust Office'}
        if f in data and data[f] in service_by_code:
            data['notes'] = f'Service Type: {service_by_code[data[f]]}'

        f2 = 'estymd'
        if f2 in data and data[f2] not in ['', 'None', 'NA', 'N/A']:
            data['notes'] += f', Established {data[f2]}'

class HealthyRideSchema(AssetSchema):
    job_code = 'healthy_ride'
#    asset_category = fields.String(dump_only=True, default='Transportation')
    asset_type = fields.String(dump_only=True, default='bike_share_stations')
    name = fields.String(load_from='station_name')
    localizability = fields.String(dump_only=True, default='fixed')
    #street_address = fields.String(load_from='property_address')
    #city = fields.String() # Currently all are in Pittsburgh, but it's
    # conceivable that Healthy Ride could someday expand into Wilkinsburg or another municipality.
    #state = fields.String(load_from='st')
    #zip_code = fields.String()
    latitude = fields.Float(load_from='latitude', allow_none=True)
    longitude = fields.Float(load_from='longitude', allow_none=True)
    #sensitive = fields.Boolean(dump_only=True, allow_none=True, default=False)
    # Include any of these or just leave them in the master table?
    #date_entered = Leave blank.
    #last_updated = # pull last_modified date from resource # 2014
    data_source_name = fields.String(default='WPRDC Dataset: Healthy Ride Stations')
    data_source_url = fields.String(default='https://data.wprdc.org/dataset/healthyride-stations')
    primary_key_from_rocket = fields.String(load_from='station_#', allow_none=False)
    notes = fields.String(allow_none=True)

    @pre_load
    def fix_notes(self, data):
        f2 = '#_of_racks'
        f = 'notes'
        if f2 in data and data[f2] not in [None, '', 'NOT AVAILABLE', 'NA', 'N/A']:
            data[f] = f'Has {data[f2]} bike racks.'

    @post_load
    def fix_key(self, data):
        assert hasattr(self, 'job_code')
        data['primary_key_from_rocket'] = form_key(self.job_code, data['primary_key_from_rocket'])

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
        'source_file': ASSET_MAP_SOURCE_DIR + 'FamilySupportCtrs-disambiguate-column-names.csv', # Has a geocoding accuracy field is being used.
        # But it also has some fields with wrong values (e.g., 'City', 'State', 'Zip') which I renamed in this modified version of the file
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FamilySupportCentersSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'], # It's not clear whether these will be fixed under updates
        # since it's just a sequence of integers. I'll call it a Possibly Unreliable Key.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'FamilySupportCtrs-disambiguate-column-names.csv',
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
        'source_file': ASSET_MAP_SOURCE_DIR + 'Allegheny_County_WIC_Vendor_Locations-nonempty-rows-transgeocoded.csv', # One empty row was manually removed.
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': WICVendorsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['objectid'], # Possibly Unreliable Key
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Allegheny_County_WIC_Vendor_Locations-nonempty-rows-transgeocoded.csv',
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
        'job_code': WICOfficesSchema().job_code, #'wic_offices',
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
        'job_code': NursingHomesSchema().job_code, #'nursing_homes',
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
        'job_code': DentistsSchema().job_code, #'dentists',
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
        'job_code': BarbersSchema().job_code, #'barbers',
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
        'job_code': NailSalonsSchema().job_code, #'nail_salons',
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
        'job_code': HairSalonsSchema().job_code, #'hair_salons',
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
        'job_code': PharmaciesSchema().job_code, #'pharmacies',
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
        'job_code': LaundromatsSchema().job_code, #'laundromats',
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
        'job_code': GasVendorsSchema().job_code, #'gas_vendors',
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
        'job_code': WMDCoffeeShopsSchema().job_code, #'wmd_coffee_shops', # Coffee shops can and should also be pulled from Geocoded Food
        # Facilities, but that might best be done manually.
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'wmd-coffee.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': WMDCoffeeShopsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['store_id'], # These primary keys are really only primary keys for the source file
        # and could fail if multiple sources are combined.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'wmd-coffee.csv',
    },
    {
        'job_code': ChildCareCentersSchema().job_code, #'child_care_providers',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Child_Care_Providers_Listing_Child_Care_Center_Allegheny.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': ChildCareCentersSchema,
        'always_clear_first': True,
        'primary_key_fields': ['license_number'], # A good primary key
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Child_Care_Providers_Listing_Child_Care_Center_Allegheny.csv',
    },
    {
        'job_code': ApartmentsSchema().job_code, #'apartments',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'apartments20plus20200117.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': ApartmentsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['parid'], # A strong primary key
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
        'job_code': ParkFacilitiesSchema().job_code, #'county_park_facilities',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Allegheny_County_Park_Facilities.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': ParkFacilitiesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['gloablid'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'Allegheny_County_Park_Facilities.csv',
    },
    {
        'job_code': CityParksSchema().job_code, #'city_parks',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'Pittsburgh_Parks.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': CityParksSchema,
        'always_clear_first': True,
        'primary_key_fields': ['globalid_2'],
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
        'job_code': 'restaurants', # GeocodedFoodFacilities categories: [Chain] Restaurant with[out] Liquor
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'GeocodedFoodFacilities-filtered-restaurants.csv', # Note that all GeocodedFoodFacilites records
        # need to be filtered not only for whether there is a closed date but for whether status == 9 (Pre-Operational), 8 (Pending), 7 (Out of Business), 0 (Inactive), 6 (Administative Closure)

        # Problems with this data: 1) There can be duplicate records for the same facility (since it's still some kind
        # of permit-based listing).

        # However, inspecting it suggests that if placard_st == 1 and status == 1, it's a legitimate, operating business.
        # Other status values may be worth including: 2 (Seasonal), 3 (Temporary), 4 (Transient), 5 (Consumer Alert)

        # 21 food facilities have placard_st == 1 but also have bus_cl_date values.
        # One of these is Stack'd in Forbes, which is open, but which the records suggest is closed.
        # Therefore the placard_st field may be more trustworthy.

        # status == 7 seems to denote a closed facility, even if placard_st == 1

        # Officially, placard_st == 1 (green), 2 (yellow), 3 (red)

        # placard_st == 0, status == 0 (test record)
        # placard_st == 0, status == 1 Senor Frog Tavern & Restaurant - operational dive bar
        # placard_st == 0, status == 1 A couple of other examples of seemingly operational facilities
        # placard_st == 0, status == 7 Shogun Japanese Restaurant - OPEN
        # placard_st == 0, status == 7 McDonald's OPEN (This one even has a 2019 business closure date.)
        # placard_st == 0 SEEMS to imply that something is OPEN. Could these be locations that were
        # temporarily shut down?

        # placard_st == 1, status == 0 Mark's Grille CLOSED
        # placard_st == 1, status == 0 Pastitsio CLOSED
        # placard_st == 1, status == 4 Au Grottens Cafe & Catering OPEN
        # placard_st == 1, status == 6 Lucia Rosa's Pizzeria CLOSED
        # placard_st == 1, status == 6 837 Grill seemingly closed
        # placard_st == 1, status == 9 Krazy Karen's Cafe & Galleria now OPEN
        # placard_st == 1, status == 9 Jersey Mike's Subs now OPEN

        # placard_st == 2, status == 1 Safari Club now CLOSED (though the yellow placard might suggest this is not necessarily so).

        # placard_st == 3, status == 1 (Winghart's, 5 Market Square) comes up as permanently closed in GM.
        # placard_st == 3, status == 0 ==> shut down by the health department

        # placard_st == 4, status == 9 CLOSED
        # placard_st == 5, status == 7 CLOSED
        # placard_st == 6, status == 6 CLOSED
        # placard_st == 7, status == 7 CLOSED
        # placard_st == NA, status == 0 Taco Bell (4032 Wm Penn Hwy, Monroeville) now OPEN, but there is another (1,1) record
        # placard_st == NA, status == 0 Fellini's Pizzeria (11748 Frankstown Rd) OPEN, but there is another (1,1) record

        # placard_st == NA, status == 1 D Pure Convenience OPEN
        # placard_st == NA, status == 1 The Ross Public House (a new version of the closed Hop House) UNCLEAR
        # placard_st == NA, status == 1 Hambone's in Lawrenceville (closed in December 2019 for unpaid taxes - now reopened)

        # Tentative conclusion: Filter out all facilities with placard_st > 1 and those with status == 7 (out of business).
        # Leave out placard_st == NA for now and try to fix later after refreshing data from source. The NA,0 records
        # seem to have corresponding 1,1 records to fill in the gap.

        # From what's left, filter out status == 0, 6
        # Keep placard_st == 1, status == 9 (pre-operational) and status == 9.
        # I'm also keeping placard_st == 1, status == 2 which is currently just two pool snack bars.

        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': GeocodedRestaurantsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'GeocodedFoodFacilities-filtered-restaurants.csv',
    },
    {
        'job_code': 'supermarkets', # GeocodedFoodFacilities categories: Chain Supermarket, Supermarket
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'GeocodedFoodFacilities-filtered-supermarkets.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': GeocodedSupermarketsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'GeocodedFoodFacilities-filtered-supermarkets.csv',
    },
    {
        'job_code': 'geocoded_food_banks', # GeocodedFoodFacilities categories: Food Banks/ Food Pantries
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'GeocodedFoodFacilities-filtered-food-banks.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': GeocodedFoodBanksSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'GeocodedFoodFacilities-filtered-food-banks.csv',
    },
    {
        'job_code': PrimaryCareSchema().job_code, #'primary_care',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'data-primary-care-access-facilities.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': PrimaryCareSchema,
        'always_clear_first': True,
        #'primary_key_fields': ['id'], # This is a weak primary key.
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'data-primary-care-access-facilities.csv',
    },
    {
        'update': 1, # Update 1 includes the notes field with the NTEE code translation and eliminates
        # locations outside of Allegheny County.
        'job_code': 'irs', #PrimaryCareSchema().job_code, #'primary_care',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'IRS_pregeocoded+Allegheny_County_Zip_Codes-just-allegheny.csv', # This source
        # file is the result of an inner join between a list of Allegheny County ZIP codes and the pre-geocoded version
        # of the original IRS file for the state of Pennsylvania.
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': IRSGeocodedSchema,
        'always_clear_first': True,
        'primary_key_fields': ['ein'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'IRS_pregeocoded+Allegheny_County_Zip_Codes-just-allegheny.csv',
    },
    {
        'update': 1, #
        'job_code': 'geocoded_social_clubs', # GeocodedFoodFacilities categories: Social Club-Bar Only
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'GeocodedFoodFacilities-filtered-social-club-bar-only.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': GeocodedSocialClubsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['id'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'GeocodedFoodFacilities-filtered-social-club-bar-only.csv',
    },
    {
        'update': 1, #
        'job_code': 'social_clubs_liquor',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'PLCBLicenseListWithSecondaries-allegheny-clubs-non-expired.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': LiquorSocialClubsSchema,
        'always_clear_first': True,
        'primary_key_fields': ['lid'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'PLCBLicenseListWithSecondaries-allegheny-clubs-non-expired.csv',
    },
    {
        'update': 1, #
        'job_code': 'post_offices',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'pa-allegheny-post-office.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': PostOfficesSchema,
        'always_clear_first': True,
        'primary_key_fields': ['fdb_id_(all)'],
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'pa-allegheny-post-office.csv',
    },
    {
        'update': 1, #
        'job_code': 'fdic',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'fdic-banks-locations-allegheny.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': FDICSchema,
        'always_clear_first': True,
        'primary_key_fields': ['uninum'], # A strong primary key
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'fdic-banks-locations-allegheny.csv',
    },
    {
        'update': 1, #
        'job_code': HealthyRideSchema().job_code, #'healthy_ride',
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'healthy-ride-station-locations-q3-2019.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': HealthyRideSchema,
        'always_clear_first': True,
        'primary_key_fields': ['station_#'], # A strong primary key
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'healthy-ride-station-locations-q3-2019.csv',
    },
    {
        'update': 1, #
        'job_code': BigBurghPantriesSchema().job_code, #'bigburgh_pantries'
        'source_type': 'local',
        'source_file': ASSET_MAP_SOURCE_DIR + 'BigBurghServices-pantries.csv',
        'encoding': 'utf-8-sig',
        #'custom_processing': conditionally_get_city_files,
        'schema': BigBurghPantriesSchema,
        'always_clear_first': True,
        #'primary_key_fields': No solid primary key
        'destinations': ['file'],
        'destination_file': ASSET_MAP_PROCESSED_DIR + 'BigBurghServices-pantries.csv',
    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.

to_geocode_or_not_to_geocode = site_is_up('geo.wprdc.org')
#to_geocode_or_not_to_geocode = False

if one_file:
    for jd in job_dicts:
        jd['destination_file'] = ASSET_MAP_PROCESSED_DIR + 'all_assets.csv'
        jd['always_clear_first'] = False
# [ ] Fix fish-fries validation by googling for how to delete rows in marshmallow schemas (or else pre-process the rows somehow... load the whole thing into memory and filter).
